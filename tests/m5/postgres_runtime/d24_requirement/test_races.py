"""Serialized two-acquirer races for D24 requirement takeover."""

from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from threading import Barrier, Event
from typing import Any

import pytest

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m5.runtime import digests
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5CancellationPlan,
    M5ExecutionEvidenceDisposition,
    M5RequirementReturnDisposition,
    M5RuntimeWorkContributionKind,
    M5TerminalReason,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from tests.m5.postgres_runtime.d24_requirement.test_recovery import (
    _acquire_and_stage_root,
    _attempt_timing,
    _forward_attempt_work,
    _full_snapshot,
    _root_result_and_output,
)


def _wait_until_expired(database: Any, deadline: Any) -> None:
    database.connection.execute(
        """
        SELECT pg_sleep(
            greatest(extract(epoch FROM (%s::timestamptz - clock_timestamp())), 0)
            + 0.02
        )
        """,
        (deadline,),
    )
    database.connection.commit()


def _lock_epoch_headers(connection: Any, *, epoch_id: int) -> None:
    row = connection.execute(
        """
        SELECT base.revision, runtime.revision
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        WHERE base.epoch_id = %s
        FOR UPDATE OF base, runtime
        """,
        (epoch_id,),
    ).fetchone()
    assert row is not None and int(row[0]) == int(row[1])


def _root_set_hash(database: Any) -> str:
    row = database.connection.execute(
        """
        SELECT requirement_root_set_hash
        FROM groundloop_m5_runtime_epoch WHERE epoch_id = %s
        """,
        (database.epoch_id,),
    ).fetchone()
    assert row is not None
    database.connection.commit()
    return str(row[0]).strip()


def _stage_all_roots(database: Any) -> int:
    store = PostgresM5RuntimeStore(database.connection)
    revision = 1
    for job in database.jobs:
        revision, _lease, _result, _output = _acquire_and_stage_root(
            database,
            store=store,
            expected_revision=revision,
            job=job,
        )
    return revision


def _prepare_barrier(database: Any) -> tuple[str, Any]:
    revision = _stage_all_roots(database)
    assert revision == 5
    root_set_hash = _root_set_hash(database)
    receipt = PostgresM5RuntimeStore(
        database.connection
    ).close_m5_requirement_roots_atomically(database.epoch_id, revision, root_set_hash)
    assert receipt.resulting_revision == 6 and not receipt.exact_replay
    return root_set_hash, receipt


@pytest.mark.parametrize("first_committer", ("timing", "mutator"))
def test_transition_timing_append_and_next_mutator_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    epoch_id = d24_requirement_db.epoch_id
    event_id = d24_requirement_db.plan.structural_event_id
    job = d24_requirement_db.jobs[0]
    contribution_kind = M5RuntimeWorkContributionKind.STRUCTURAL_OPEN
    contribution_key = digests.runtime_work_contribution_key_digest(
        epoch_id=epoch_id,
        contribution_kind=contribution_kind,
        source_id=event_id,
    )

    def append(connection: Any, observed: Any) -> Any:
        return PostgresM5RuntimeStore(connection).append_transition_call_timing(
            epoch_id,
            contribution_kind,
            event_id,
            contribution_key,
            1,
            observed,
        )

    def acquire(connection: Any) -> Any:
        return PostgresM5RuntimeStore(connection).acquire_m5_job(epoch_id, 1, job)

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "timing":
                acquire_started = Event()

                def blocked_acquire() -> Any:
                    acquire_started.set()
                    return acquire(right)

                with left.transaction():
                    timing_receipt = append(left, _attempt_timing())
                    assert not timing_receipt.exact_replay
                    acquire_future = pool.submit(blocked_acquire)
                    assert acquire_started.wait(timeout=5)
                    assert not acquire_future.done()
                lease = acquire_future.result(timeout=10)
                assert lease.resulting_revision == 2
                timing, coverage = PostgresM5RuntimeStore(
                    d24_requirement_db.connection
                ).current_event_timing(epoch_id)
                observed = _attempt_timing()
                assert (
                    timing.coordinator_non_db_non_neural_ns,
                    timing.neural_wall_ns,
                    timing.postgres_roundtrip_wall_ns,
                    timing.external_io_wall_ns,
                    timing.end_to_end_wall_ns,
                ) == (
                    observed.coordinator_non_db_non_neural_ns,
                    observed.neural_wall_ns,
                    observed.postgres_roundtrip_wall_ns,
                    observed.external_io_wall_ns,
                    observed.end_to_end_wall_ns,
                )
                assert timing.postgres_server_execution_ns is None
                assert timing.postgres_lock_wait_ns is None
                assert timing.postgres_wal_bytes is None
                assert timing.postgres_shared_block_reads is None
                assert (
                    coverage.required_expected_count,
                    coverage.required_observed_count,
                    coverage.required_missing_count,
                ) == (2, 1, 1)
            else:
                append_started = Event()

                def blocked_missing_append() -> Any:
                    append_started.set()
                    return append(right, None)

                with left.transaction():
                    lease = acquire(left)
                    assert lease.resulting_revision == 2
                    append_future = pool.submit(blocked_missing_append)
                    assert append_started.wait(timeout=5)
                    assert not append_future.done()
                timing_receipt = append_future.result(timeout=10)
                assert timing_receipt.exact_replay
                assert timing_receipt.resulting_revision == 2

                before_conflict = _full_snapshot(d24_requirement_db.connection)
                with pytest.raises(EventConflictError, match="immutable observation"):
                    append(d24_requirement_db.connection, _attempt_timing())
                assert _full_snapshot(d24_requirement_db.connection) == before_conflict
                timing, coverage = PostgresM5RuntimeStore(
                    d24_requirement_db.connection
                ).current_event_timing(epoch_id)
                assert timing.coordinator_non_db_non_neural_ns == 0
                assert (
                    coverage.required_expected_count,
                    coverage.required_observed_count,
                    coverage.required_missing_count,
                ) == (2, 0, 2)


def test_two_expired_acquirers_create_one_dense_successor(
    d24_requirement_db: Any,
) -> None:
    job = d24_requirement_db.jobs[0]
    first = PostgresM5RuntimeStore(d24_requirement_db.connection).acquire_m5_job(
        d24_requirement_db.epoch_id, 1, job
    )
    assert first.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db, first.lease_expires_at)

    with d24_requirement_db.two_connections() as (left, right):
        start = Barrier(2)

        def acquire(connection: Any) -> Any:
            start.wait()
            return PostgresM5RuntimeStore(connection).acquire_m5_job(
                d24_requirement_db.epoch_id, 2, job
            )

        with ThreadPoolExecutor(max_workers=2) as pool:
            futures = (pool.submit(acquire, left), pool.submit(acquire, right))
            leases = tuple(future.result(timeout=10) for future in futures)

    assert {lease.disposition for lease in leases} == {
        M5AcquisitionDisposition.DISPATCH_TAKEOVER,
        M5AcquisitionDisposition.LIVE_LEASE,
    }
    takeover = next(
        lease
        for lease in leases
        if lease.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    )
    live = next(
        lease
        for lease in leases
        if lease.disposition is M5AcquisitionDisposition.LIVE_LEASE
    )
    assert takeover.attempt is not None
    assert live.attempt == takeover.attempt
    assert takeover.resulting_revision == live.resulting_revision == 3
    assert takeover.should_execute
    assert not live.should_execute

    rows = d24_requirement_db.connection.execute(
        """
        SELECT attempt_ordinal, attempt_state
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (job.logical_job_id,),
    ).fetchall()
    assert rows == [(1, "expired"), (2, "dispatched")]
    counts = d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_dispatch_record
           WHERE epoch_id = %s AND subgraph = 'requirement'),
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'm5_acquisition')
        """,
        (d24_requirement_db.epoch_id, d24_requirement_db.epoch_id),
    ).fetchone()
    assert counts == (2, 2)


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize("first_committer", ("barrier", "final_root"))
def test_final_root_and_barrier_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    """Falsify partial closure across both final-root/barrier serial orders."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    revision, _lease, _result, _output = _acquire_and_stage_root(
        d24_requirement_db,
        store=store,
        expected_revision=1,
        job=d24_requirement_db.jobs[0],
    )
    assert revision == 3
    final_job = d24_requirement_db.jobs[1]
    final_lease = store.acquire_m5_job(d24_requirement_db.epoch_id, revision, final_job)
    assert final_lease.attempt is not None and final_lease.resulting_revision == 4
    result, output = _root_result_and_output(
        d24_requirement_db, job=final_job, attempt=final_lease.attempt
    )
    root_set_hash = _root_set_hash(d24_requirement_db)

    def stage_final(connection: Any, failure_injector: Any = None) -> Any:
        return PostgresM5RuntimeStore(connection).stage_m5_discovery_result_atomically(
            d24_requirement_db.epoch_id,
            4,
            final_lease,
            final_job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
            failure_injector=failure_injector,
        )

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=2) as pool:
            if first_committer == "barrier":
                stage_started = Event()

                def blocked_stage() -> Any:
                    stage_started.set()
                    return stage_final(right)

                with left.transaction():
                    _lock_epoch_headers(left, epoch_id=d24_requirement_db.epoch_id)
                    stage_future = pool.submit(blocked_stage)
                    assert stage_started.wait(timeout=5)
                    with pytest.raises(InvalidEventError, match="incomplete"):
                        PostgresM5RuntimeStore(
                            left
                        ).close_m5_requirement_roots_atomically(
                            d24_requirement_db.epoch_id,
                            4,
                            root_set_hash,
                        )
                stage_receipt = stage_future.result(timeout=10)
            else:
                stage_locked = Event()
                release_stage = Event()

                def pause_stage(point: str) -> None:
                    if point == "root_stage_authorized":
                        stage_locked.set()
                        assert release_stage.wait(timeout=5)

                stage_future = pool.submit(stage_final, left, pause_stage)
                assert stage_locked.wait(timeout=5)
                barrier_started = Event()

                def blocked_barrier() -> Any:
                    barrier_started.set()
                    return PostgresM5RuntimeStore(
                        right
                    ).close_m5_requirement_roots_atomically(
                        d24_requirement_db.epoch_id,
                        4,
                        root_set_hash,
                    )

                barrier_future = pool.submit(blocked_barrier)
                assert barrier_started.wait(timeout=5)
                release_stage.set()
                stage_receipt = stage_future.result(timeout=10)
                with pytest.raises(EventConflictError, match="stale"):
                    barrier_future.result(timeout=10)

    assert (
        stage_receipt.disposition is M5RequirementReturnDisposition.APPLIED
        and stage_receipt.resulting_revision == 5
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT count(*) FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s AND contribution_kind = 'root_barrier'
        """,
        (d24_requirement_db.epoch_id,),
    ).fetchone() == (0,)
    d24_requirement_db.connection.commit()
    barrier = store.close_m5_requirement_roots_atomically(
        d24_requirement_db.epoch_id, 5, root_set_hash
    )
    assert barrier.resulting_revision == 6 and not barrier.exact_replay


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize("first_committer", ("barrier_replay", "verifier"))
def test_barrier_replay_and_verifier_acquisition_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    root_set_hash, _barrier = _prepare_barrier(d24_requirement_db)
    verifier_job = PostgresM5RuntimeStore(d24_requirement_db.connection).verifier_jobs(
        d24_requirement_db.epoch_id
    )[0]

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "barrier_replay":
                verifier_started = Event()

                def blocked_verifier() -> Any:
                    verifier_started.set()
                    return PostgresM5RuntimeStore(right).acquire_m5_job(
                        d24_requirement_db.epoch_id, 6, verifier_job
                    )

                with left.transaction():
                    _lock_epoch_headers(left, epoch_id=d24_requirement_db.epoch_id)
                    verifier_future = pool.submit(blocked_verifier)
                    assert verifier_started.wait(timeout=5)
                    replay = PostgresM5RuntimeStore(
                        left
                    ).close_m5_requirement_roots_atomically(
                        d24_requirement_db.epoch_id,
                        5,
                        root_set_hash,
                    )
                verifier_lease = verifier_future.result(timeout=10)
                assert replay.resulting_revision == 6
            else:
                replay_started = Event()

                def blocked_replay() -> Any:
                    replay_started.set()
                    return PostgresM5RuntimeStore(
                        right
                    ).close_m5_requirement_roots_atomically(
                        d24_requirement_db.epoch_id,
                        5,
                        root_set_hash,
                    )

                with left.transaction():
                    _lock_epoch_headers(left, epoch_id=d24_requirement_db.epoch_id)
                    verifier_lease = PostgresM5RuntimeStore(left).acquire_m5_job(
                        d24_requirement_db.epoch_id, 6, verifier_job
                    )
                    replay_future = pool.submit(blocked_replay)
                    assert replay_started.wait(timeout=5)
                replay = replay_future.result(timeout=10)
                assert replay.resulting_revision == 7

    assert replay.exact_replay
    assert (
        verifier_lease.disposition is M5AcquisitionDisposition.DISPATCH_NEW
        and verifier_lease.resulting_revision == 7
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_dispatch_record
           WHERE epoch_id = %s AND logical_job_id = %s),
          (SELECT count(*) FROM groundloop_m5_runtime_work_contribution
           WHERE epoch_id = %s AND contribution_kind = 'm5_acquisition'
             AND applied_revision = 7)
        """,
        (
            d24_requirement_db.epoch_id,
            verifier_job.logical_job_id,
            d24_requirement_db.epoch_id,
        ),
    ).fetchone() == (1, 1)


@pytest.mark.parametrize("d24_requirement_db", ["two_roots"], indirect=True)
@pytest.mark.parametrize("first_committer", ("barrier", "cancellation"))
def test_barrier_and_cancellation_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    """The production barrier and cancellation routes serialize both ways."""

    assert _stage_all_roots(d24_requirement_db) == 5
    root_set_hash = _root_set_hash(d24_requirement_db)
    target_job = d24_requirement_db.jobs[0]
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=d24_requirement_db.epoch_id,
        cancelled_job_ids=(target_job.logical_job_id,),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )

    def cancel(connection: Any) -> Any:
        return PostgresM5RuntimeStore(connection).cancel_m5_work_atomically(
            d24_requirement_db.epoch_id,
            5,
            plan,
        )

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "barrier":
                cancel_started = Event()

                def blocked_cancel() -> Any:
                    cancel_started.set()
                    return cancel(right)

                with left.transaction():
                    _lock_epoch_headers(left, epoch_id=d24_requirement_db.epoch_id)
                    cancel_future = pool.submit(blocked_cancel)
                    assert cancel_started.wait(timeout=5)
                    barrier = PostgresM5RuntimeStore(
                        left
                    ).close_m5_requirement_roots_atomically(
                        d24_requirement_db.epoch_id,
                        5,
                        root_set_hash,
                    )
                with pytest.raises(EventConflictError, match="stale"):
                    cancel_future.result(timeout=10)
                assert barrier.resulting_revision == 6
            else:
                barrier_started = Event()

                def blocked_barrier() -> Any:
                    barrier_started.set()
                    return PostgresM5RuntimeStore(
                        right
                    ).close_m5_requirement_roots_atomically(
                        d24_requirement_db.epoch_id,
                        5,
                        root_set_hash,
                    )

                with left.transaction():
                    cancellation = cancel(left)
                    barrier_future = pool.submit(blocked_barrier)
                    assert barrier_started.wait(timeout=5)
                assert cancellation.resulting_revision == 6
                assert not cancellation.exact_replay
                with pytest.raises(EventConflictError, match="stale"):
                    barrier_future.result(timeout=10)

    contribution_counts = d24_requirement_db.connection.execute(
        """
        SELECT
          count(*) FILTER (WHERE contribution_kind = 'root_barrier'),
          count(*) FILTER (WHERE contribution_kind = 'cancellation')
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s
        """,
        (d24_requirement_db.epoch_id,),
    ).fetchone()
    if first_committer == "barrier":
        assert contribution_counts == (1, 0)
    else:
        assert contribution_counts == (0, 1)


@pytest.mark.parametrize("first_committer", ("cancellation", "result"))
def test_root_result_and_cancellation_serialize_in_both_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    lease = store.acquire_m5_job(epoch_id, 1, job)
    assert lease.attempt is not None and lease.resulting_revision == 2
    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=lease.attempt
    )
    d24_requirement_db.connection.commit()
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=(job.logical_job_id,),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )

    def stage(connection: Any) -> Any:
        return PostgresM5RuntimeStore(connection).stage_m5_discovery_result_atomically(
            epoch_id,
            2,
            lease,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    def cancel(connection: Any) -> Any:
        return PostgresM5RuntimeStore(connection).cancel_m5_work_atomically(
            epoch_id, 2, plan
        )

    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "cancellation":
                stage_started = Event()

                def blocked_stage() -> Any:
                    stage_started.set()
                    return stage(right)

                with left.transaction():
                    cancellation = cancel(left)
                    stage_future = pool.submit(blocked_stage)
                    assert stage_started.wait(timeout=5)
                with pytest.raises(EventConflictError, match="stale preterminal"):
                    stage_future.result(timeout=10)
                stage_receipt = store.stage_m5_discovery_result_atomically(
                    epoch_id,
                    cancellation.resulting_revision,
                    lease,
                    job,
                    result,
                    output,
                    M5ExecutionEvidenceDisposition.RETURNED,
                    _forward_attempt_work(),
                    _attempt_timing(),
                    eligible_snapshot_exhausted=True,
                )
                assert (
                    stage_receipt.disposition
                    is M5RequirementReturnDisposition.TERMINAL_AUDIT_PRETERMINAL
                    and stage_receipt.resulting_revision == 3
                )
            else:
                cancel_started = Event()

                def blocked_cancel() -> Any:
                    cancel_started.set()
                    return cancel(right)

                with left.transaction():
                    stage_receipt = stage(left)
                    cancel_future = pool.submit(blocked_cancel)
                    assert cancel_started.wait(timeout=5)
                with pytest.raises(EventConflictError, match="stale"):
                    cancel_future.result(timeout=10)
                cancellation = store.cancel_m5_work_atomically(
                    epoch_id, stage_receipt.resulting_revision, plan
                )
                assert cancellation.resulting_revision == 4

    row = d24_requirement_db.connection.execute(
        """
        SELECT runtime.revision, job.job_state, job.result_artifact_id,
               job.result_artifact_hash, scope.scope_state,
               scope.staged_result_artifact_hash,
               work.requirement_cancelled_job_count,
               count(contribution.*) FILTER (
                   WHERE contribution.contribution_kind = 'cancellation'),
               count(contribution.*) FILTER (
                   WHERE contribution.contribution_kind = 'root_result_stage'),
               count(contribution.*) FILTER (
                   WHERE contribution.contribution_kind =
                         'preterminal_late_return')
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job
          ON job.epoch_id = runtime.epoch_id
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        JOIN groundloop_m5_runtime_work_accumulator AS work
          ON work.epoch_id = runtime.epoch_id
        LEFT JOIN groundloop_m5_runtime_work_contribution AS contribution
          ON contribution.epoch_id = runtime.epoch_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        GROUP BY runtime.revision, job.job_state, job.result_artifact_id,
                 job.result_artifact_hash, scope.scope_state,
                 scope.staged_result_artifact_hash,
                 work.requirement_cancelled_job_count
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone()
    assert row is not None and row[1:5] == ("cancelled", None, None, "cancelled")
    assert row[6] == 1 and row[7] == 1
    if first_committer == "cancellation":
        assert row[0] == 3 and row[5] is None and row[8:] == (0, 1)
    else:
        assert row[0] == 4
        assert str(row[5]).strip() == result.result_artifact_hash
        assert row[8:] == (1, 0)


@pytest.mark.parametrize("first_committer", ("expired_output", "cancellation"))
def test_expired_output_and_cancellation_follow_c3_serial_orders(
    d24_requirement_db: Any,
    first_committer: str,
) -> None:
    """Exercise both accepted C3 preterminal outcomes on production routes."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db, original.lease_expires_at)
    successor = store.acquire_m5_job(epoch_id, 2, job)
    assert successor.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert successor.attempt is not None and successor.resulting_revision == 3
    assert successor.attempt.attempt_ordinal == original.attempt.attempt_ordinal + 1
    assert successor.attempt.attempt_id != original.attempt.attempt_id

    attempt_rows_before = d24_requirement_db.connection.execute(
        """
        SELECT xmin::text, attempt_id, attempt_ordinal, attempt_state,
               attempt_output_digest, lease_token_hash, lease_expires_at
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (job.logical_job_id,),
    ).fetchall()
    d24_requirement_db.connection.commit()
    assert [(row[2], row[3]) for row in attempt_rows_before] == [
        (1, "expired"),
        (2, "dispatched"),
    ]

    result, output = _root_result_and_output(
        d24_requirement_db, job=job, attempt=original.attempt
    )
    plan = M5CancellationPlan.build(
        structural_event_id=d24_requirement_db.plan.structural_event_id,
        epoch_id=epoch_id,
        cancelled_job_ids=(job.logical_job_id,),
        reason=M5TerminalReason.SCOPE_RETIRED,
    )

    def stage(connection: Any, expected_revision: int) -> Any:
        return PostgresM5RuntimeStore(connection).stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            result,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    def cancel(connection: Any) -> Any:
        return PostgresM5RuntimeStore(connection).cancel_m5_work_atomically(
            epoch_id, 3, plan
        )

    def archived_sidecar_snapshot(connection: Any) -> tuple[Any, ...]:
        assert original.attempt is not None
        row = connection.execute(
            """
            SELECT artifact.xmin::text, to_jsonb(artifact)::text,
                   expired.xmin::text, to_jsonb(expired)::text,
                   evidence.xmin::text, to_jsonb(evidence)::text
            FROM groundloop_m5_attempt_result_artifact AS artifact
            JOIN groundloop_m5_expired_attempt_return AS expired
              ON expired.epoch_id = artifact.job_epoch_id
             AND expired.attempt_id = artifact.attempt_id
             AND expired.subgraph = 'requirement'
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.epoch_id = artifact.job_epoch_id
             AND evidence.attempt_id = artifact.attempt_id
             AND evidence.subgraph = 'requirement'
            WHERE artifact.attempt_id = %s
            """,
            (original.attempt.attempt_id,),
        ).fetchone()
        assert row is not None
        return tuple(row)

    expired_receipt = None
    archive_before_cancellation = None
    with d24_requirement_db.two_connections() as (left, right):
        with ThreadPoolExecutor(max_workers=1) as pool:
            if first_committer == "expired_output":
                cancellation_started = Event()

                def blocked_cancellation() -> Any:
                    cancellation_started.set()
                    return cancel(right)

                with left.transaction():
                    expired_receipt = stage(left, 3)
                    assert (
                        expired_receipt.disposition
                        is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
                    )
                    assert expired_receipt.resulting_revision == 3
                    archive_before_cancellation = archived_sidecar_snapshot(left)
                    cancellation_future = pool.submit(blocked_cancellation)
                    assert cancellation_started.wait(timeout=5)
                    assert not cancellation_future.done()
                cancellation = cancellation_future.result(timeout=10)
            else:
                output_started = Event()

                def blocked_output() -> Any:
                    output_started.set()
                    return stage(right, 3)

                with left.transaction():
                    cancellation = cancel(left)
                    output_future = pool.submit(blocked_output)
                    assert output_started.wait(timeout=5)
                    assert not output_future.done()
                with pytest.raises(
                    EventConflictError,
                    match="nonrunning successor requires terminal event",
                ):
                    output_future.result(timeout=10)

    assert cancellation.resulting_revision == 4 and not cancellation.exact_replay
    assert cancellation.cancelled_job_ids == (job.logical_job_id,)

    attempt_rows_after = d24_requirement_db.connection.execute(
        """
        SELECT xmin::text, attempt_id, attempt_ordinal, attempt_state,
               attempt_output_digest, lease_token_hash, lease_expires_at
        FROM groundloop_m5_job_attempt
        WHERE logical_job_id = %s
        ORDER BY attempt_ordinal
        """,
        (job.logical_job_id,),
    ).fetchall()
    assert attempt_rows_after == attempt_rows_before

    closure = d24_requirement_db.connection.execute(
        """
        SELECT base.revision, runtime.revision, runtime.open_work_count,
               runtime.open_scope_count, job.job_state, scope.scope_state,
               work.requirement_cancelled_job_count,
               (SELECT count(*)
                FROM groundloop_m5_runtime_work_contribution AS contribution
                WHERE contribution.epoch_id = runtime.epoch_id
                  AND contribution.contribution_kind = 'cancellation'),
               (SELECT count(*)
                FROM groundloop_m5_attempt_result_artifact AS artifact
                WHERE artifact.attempt_id = %s),
               (SELECT count(*)
                FROM groundloop_m5_expired_attempt_return AS expired
                WHERE expired.epoch_id = runtime.epoch_id
                  AND expired.attempt_id = %s
                  AND expired.subgraph = 'requirement'),
               (SELECT count(*)
                FROM groundloop_m5_attempt_execution_evidence AS evidence
                WHERE evidence.epoch_id = runtime.epoch_id
                  AND evidence.attempt_id = %s
                  AND evidence.subgraph = 'requirement'),
               (SELECT count(*)
                FROM groundloop_m5_runtime_work_contribution AS contribution
                WHERE contribution.epoch_id = runtime.epoch_id
                  AND contribution.source_id = %s
                  AND contribution.contribution_kind =
                      'preterminal_late_return')
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_semantic_job AS job
          ON job.epoch_id = runtime.epoch_id
         AND job.logical_job_id = %s
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        JOIN groundloop_m5_runtime_work_accumulator AS work
          ON work.epoch_id = runtime.epoch_id
        WHERE base.epoch_id = %s
        """,
        (
            original.attempt.attempt_id,
            original.attempt.attempt_id,
            original.attempt.attempt_id,
            original.attempt.attempt_id,
            job.logical_job_id,
            epoch_id,
        ),
    ).fetchone()
    expected_audit_count = 1 if first_committer == "expired_output" else 0
    assert closure == (
        4,
        4,
        0,
        0,
        "cancelled",
        "cancelled",
        1,
        1,
        expected_audit_count,
        expected_audit_count,
        expected_audit_count,
        expected_audit_count,
    )
    pending_rows = d24_requirement_db.connection.execute(
        """
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        UNION ALL
        SELECT broad_reverse_scope_count, forward_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        """,
        (epoch_id, epoch_id),
    ).fetchall()
    assert pending_rows
    assert all(tuple(row) == (0, 0, 0, 0, 4) for row in pending_rows)

    if first_committer == "expired_output":
        assert expired_receipt is not None
        assert archive_before_cancellation is not None
        assert (
            archived_sidecar_snapshot(d24_requirement_db.connection)
            == archive_before_cancellation
        )
        archive = d24_requirement_db.connection.execute(
            """
            SELECT artifact.attempt_output_digest,
                   artifact.result_artifact_hash,
                   artifact.job_state_at_receipt,
                   artifact.job_state_after,
                   artifact.archive_reason,
                   artifact.cancelled_by_event_id,
                   artifact.cancelled_by_epoch_id,
                   artifact.cancellation_reason,
                   expired.worker_output_digest,
                   expired.worker_artifact_hash,
                   expired.received_after_terminal,
                   evidence.disposition
            FROM groundloop_m5_attempt_result_artifact AS artifact
            JOIN groundloop_m5_expired_attempt_return AS expired
              ON expired.epoch_id = artifact.job_epoch_id
             AND expired.attempt_id = artifact.attempt_id
             AND expired.subgraph = 'requirement'
            JOIN groundloop_m5_attempt_execution_evidence AS evidence
              ON evidence.epoch_id = artifact.job_epoch_id
             AND evidence.attempt_id = artifact.attempt_id
             AND evidence.subgraph = 'requirement'
            WHERE artifact.attempt_id = %s
            """,
            (original.attempt.attempt_id,),
        ).fetchone()
        assert archive == (
            output.attempt_output_digest,
            output.result_artifact_hash,
            "running",
            "running",
            "attempt_expired",
            None,
            None,
            None,
            output.attempt_output_digest,
            output.result_artifact_hash,
            False,
            "returned",
        )
        timing_rows = d24_requirement_db.connection.execute(
            """
            SELECT transition.contribution_kind, transition.source_id,
                   transition.anchor_revision,
                   transition.required_interval_observed,
                   accumulator.pending_contribution_kind,
                   accumulator.pending_source_id,
                   accumulator.pending_anchor_revision,
                   accumulator.updated_revision
            FROM groundloop_m5_transition_call_timing AS transition
            JOIN groundloop_m5_runtime_timing_accumulator AS accumulator
              ON accumulator.epoch_id = transition.epoch_id
            WHERE transition.epoch_id = %s
              AND transition.contribution_kind = 'preterminal_late_return'
              AND transition.source_id = %s
            """,
            (epoch_id, original.attempt.attempt_id),
        ).fetchone()
        assert timing_rows == (
            "preterminal_late_return",
            original.attempt.attempt_id,
            3,
            False,
            "cancellation",
            plan.plan_digest,
            4,
            4,
        )
        before_replay = _full_snapshot(d24_requirement_db.connection)
        replay = stage(d24_requirement_db.connection, 4)
        assert replay.exact_replay
        assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_PRETERMINAL
        assert _full_snapshot(d24_requirement_db.connection) == before_replay
    else:
        for rejected_revision in (3, 4):
            before_retry = _full_snapshot(d24_requirement_db.connection)
            with pytest.raises(
                EventConflictError,
                match="nonrunning successor requires terminal event",
            ):
                stage(d24_requirement_db.connection, rejected_revision)
            assert _full_snapshot(d24_requirement_db.connection) == before_retry
