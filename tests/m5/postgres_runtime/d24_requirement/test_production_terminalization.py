"""Live production acceptance tests for D24 R2a failure terminalization."""

from __future__ import annotations

import hashlib
from typing import Any

import pytest
from psycopg import Connection, sql

from groundloop.errors import EventConflictError, InvalidEventError
from groundloop.m5.events import (
    ReplaceGroupEvent,
    RetireGroupEvent,
    m5_event_payload_digest,
)
from groundloop.m5.runtime.contracts import (
    M5AcquisitionDisposition,
    M5CandidatePolicyManifest,
    M5DiscoveryDirection,
    M5DiscoveryScopeContract,
    M5ExecutionEvidenceDisposition,
    M5JobKind,
    M5LogicalJobSpec,
    M5ReplayedOutcome,
    M5RequirementReturnDisposition,
    M5RunFailureReason,
    M5RunState,
    M5RuntimeTiming,
    M5RuntimeTimingCoverage,
    M5RuntimeWork,
    M5TypedEventPlan,
    RequirementRegistrySnapshot,
    RequirementRegistrySnapshotEntry,
)
from groundloop.m5.runtime.persistence import PostgresM5RuntimeStore
from tests.m5.postgres.helpers import make_group
from tests.m5.postgres_runtime.d24_requirement.test_recovery import (
    _attempt_timing,
    _complete_verifier,
    _forward_attempt_work,
    _prepare_verifier_attempt,
    _root_result_and_output,
    _verifier_attempt_work,
    _verifier_semantic_closure_counts,
    _wait_until_expired,
)


class InjectedTerminalizationFailure(RuntimeError):
    """Deterministic test-only failure at one production cutoff."""


class RollBackDirectKindFixture(RuntimeError):
    """Intentionally roll back one test-local persisted-kind substitution."""


FAILURE_TRANSITION_POINTS = (
    "typed_fail_jobs_locked",
    "typed_fail_attempts_locked",
    "typed_fail_accounting_started",
    "typed_fail_authorized",
    "typed_fail_jobs_cancelled",
    "typed_fail_scopes_closed",
    "typed_fail_counters_updated",
    "typed_fail_structure_failed",
    "typed_fail_cancellation_contribution_inserted",
    "typed_fail_epoch_failure_contribution_inserted",
    "typed_fail_base_updated",
    "typed_fail_runtime_updated",
    "typed_fail_work_accumulator_terminalized",
    "typed_fail_timing_accumulator_terminalized",
    "typed_fail_work_inserted",
    "typed_fail_result_inserted",
    "typed_fail_timing_coverage_inserted",
    "typed_fail_before_constraints",
    "typed_fail_after_constraints",
)

_PUBLISHED_UNCHANGED_TABLES = (
    "groundloop_m4_publication_head",
    "groundloop_m5_publication_head",
    "groundloop_m5_group_validity",
    "groundloop_m5_group_family_retirement",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_status_delta",
    "groundloop_claim_state_materialized",
    "groundloop_answer_state_materialized",
    "groundloop_claim_certificate",
    "groundloop_published_claim_state",
    "groundloop_published_answer_state",
    "groundloop_m5_requirement_state_materialized",
    "groundloop_m5_group_state_materialized",
    "groundloop_m5_claim_state_materialized",
    "groundloop_m5_answer_state_materialized",
    "groundloop_m5_published_requirement_state",
    "groundloop_m5_published_group_state",
    "groundloop_m5_published_claim_state",
    "groundloop_m5_published_answer_state",
    "groundloop_m5_group_certificate_artifact",
    "groundloop_m5_group_certificate_artifact_row",
    "groundloop_m5_claim_certificate_artifact",
    "groundloop_m5_published_group_certificate_binding",
    "groundloop_m5_published_claim_certificate_binding",
)

_FROZEN_TERMINAL_TABLES = (
    "groundloop_epoch",
    "groundloop_m5_runtime_epoch",
    "groundloop_m5_semantic_job",
    "groundloop_m5_discovery_scope",
    "groundloop_m5_owner_pending_counter",
    "groundloop_m5_answer_pending_counter",
    "groundloop_m5_group_family",
    "groundloop_m5_group_version",
    "groundloop_m5_requirement_version",
    "groundloop_m5_group_deactivation",
    "groundloop_m5_runtime_work_contribution",
    "groundloop_m5_runtime_work_accumulator",
    "groundloop_m5_runtime_timing_contribution",
    "groundloop_m5_transition_call_timing",
    "groundloop_m5_runtime_timing_accumulator",
    "groundloop_m5_runtime_work",
    "groundloop_m5_event_result",
    "groundloop_m5_event_result_delta",
    "groundloop_m5_event_result_state_reference",
    "groundloop_m5_event_timing_coverage",
    "groundloop_m5_postcommit_invocation_telemetry",
    "groundloop_m5_requirement_admitted_pair",
    "groundloop_m5_requirement_admitted_pair_source",
    "groundloop_m5_requirement_pair_input",
    "groundloop_m5_requirement_verifier_artifact",
    "groundloop_m5_requirement_verifier_execution",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
    "groundloop_m5_working_currency_history",
)

_LATE_SEMANTIC_TABLES = (
    "groundloop_m5_requirement_channel_hit",
    "groundloop_m5_requirement_scope_selection",
    "groundloop_m5_requirement_discovery_result",
    "groundloop_m5_requirement_admitted_pair",
    "groundloop_m5_requirement_admitted_pair_source",
    "groundloop_m5_requirement_pair_input",
    "groundloop_m5_requirement_verifier_artifact",
    "groundloop_m5_requirement_verifier_execution",
    "groundloop_semantic_observation",
    "groundloop_observation_currency",
    "groundloop_published_observation_currency",
    "groundloop_working_observation_delta",
    "groundloop_m5_working_currency_history",
)


def _sha(value: str) -> str:
    return hashlib.sha256(value.encode("utf-8")).hexdigest()


def _table_names(connection: Connection[Any]) -> tuple[str, ...]:
    return tuple(
        str(row[0])
        for row in connection.execute(
            """
            SELECT tablename
            FROM pg_tables
            WHERE schemaname = current_schema()
            ORDER BY tablename COLLATE "C"
            """
        ).fetchall()
    )


def _database_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Capture logical rows plus xmin so no-op rewrites remain observable."""

    connection.commit()
    snapshot = []
    for table_name in _table_names(connection):
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        snapshot.append((table_name, tuple((str(row[0]), str(row[1])) for row in rows)))
    connection.commit()
    return tuple(snapshot)


def _database_snapshot_in_transaction(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    """Capture the current transaction image without committing fixture setup."""

    snapshot = []
    for table_name in _table_names(connection):
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        snapshot.append((table_name, tuple((str(row[0]), str(row[1])) for row in rows)))
    return tuple(snapshot)


def _strict_published_snapshot(
    connection: Connection[Any],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    snapshot = list(_snapshot_tables(connection, _PUBLISHED_UNCHANGED_TABLES))
    for table_name in (
        "groundloop_m5_group_family",
        "groundloop_m5_group_version",
        "groundloop_m5_requirement_version",
    ):
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row WHERE lifecycle_state = 'PUBLISHED' "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        snapshot.append(
            (
                f"{table_name}:PUBLISHED",
                tuple((str(row[0]), str(row[1])) for row in rows),
            )
        )
    connection.commit()
    return tuple(snapshot)


def _snapshot_tables(
    connection: Connection[Any],
    table_names: tuple[str, ...],
) -> tuple[tuple[str, tuple[tuple[str, str], ...]], ...]:
    connection.commit()
    snapshot = []
    for table_name in table_names:
        rows = connection.execute(
            sql.SQL(
                "SELECT xmin::text, to_jsonb(snapshot_row)::text "
                "FROM {} AS snapshot_row "
                'ORDER BY to_jsonb(snapshot_row)::text COLLATE "C", xmin::text'
            ).format(sql.Identifier(table_name))
        ).fetchall()
        snapshot.append((table_name, tuple((str(row[0]), str(row[1])) for row in rows)))
    connection.commit()
    return tuple(snapshot)


def _sum_work(*parts: M5RuntimeWork) -> M5RuntimeWork:
    return M5RuntimeWork(
        **{
            name: sum(getattr(part, name) for part in parts)
            for name in M5RuntimeWork.counter_names()
        }
    )


def _requirement_snapshot(group: Any) -> RequirementRegistrySnapshot:
    return RequirementRegistrySnapshot.build(
        tuple(
            RequirementRegistrySnapshotEntry.build(
                requirement_version_id=requirement.requirement_version_id,
                group_version_id=group.group_version_id,
                group_family_id=group.group_family_id,
                owner_claim_id=group.owner_claim_id,
                requirement_text=requirement.requirement_text,
            )
            for requirement in group.requirements
        )
    )


def _root_jobs_for_plan(
    plan: M5TypedEventPlan,
    manifest: M5CandidatePolicyManifest,
) -> tuple[M5LogicalJobSpec, ...]:
    event = plan.event
    requirements = (
        event.successor.requirements if isinstance(event, ReplaceGroupEvent) else ()
    )
    jobs = []
    for requirement in requirements:
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
    return tuple(sorted(jobs, key=lambda item: item.logical_job_id))


def _replacement_or_retirement_plan(
    database: Any,
    event_kind: str,
) -> tuple[M5TypedEventPlan, tuple[M5LogicalJobSpec, ...]]:
    row = database.connection.execute(
        """
        SELECT group_version.group_version_id, group_version.group_family_id,
               family.claim_id, requirement.requirement_version_id
        FROM groundloop_m5_group_version AS group_version
        JOIN groundloop_m5_group_family AS family USING (group_family_id)
        JOIN groundloop_m5_group_validity AS validity USING (group_version_id)
        JOIN groundloop_m5_requirement_version AS requirement USING (group_version_id)
        WHERE group_version.lifecycle_state = 'PUBLISHED'
          AND family.lifecycle_state = 'PUBLISHED'
          AND validity.valid_to_epoch IS NULL
        ORDER BY requirement.ordinal
        LIMIT 1
        """
    ).fetchone()
    assert row is not None
    old_group_id, family_id, owner_claim_id, old_requirement_id = map(str, row)
    event_id = f"d24-r2a-{event_kind}"
    if event_kind == "replace":
        successor = make_group(
            group_id="d24-r2a-replacement-v2",
            family_id=family_id,
            claim_id=owner_claim_id,
            texts=("replacement required fact",),
            requirement_ids=("d24-r2a-replacement-requirement-v2",),
            predecessors=(old_requirement_id,),
            supersedes_group_id=old_group_id,
            source_id="d24-r2a-replacement",
        )
        event = ReplaceGroupEvent(
            event_id=event_id,
            old_group_version_id=old_group_id,
            successor=successor,
        )
        snapshot = _requirement_snapshot(successor)
    else:
        event = RetireGroupEvent(event_id=event_id, group_version_id=old_group_id)
        snapshot = RequirementRegistrySnapshot.build(())
    plan = M5TypedEventPlan(
        structural_event_id=event_id,
        event=event,
        payload_hash=m5_event_payload_digest(event),
        direct_plan=None,
        candidate_policy_id=database.manifest.candidate_policy_id,
        candidate_policy_manifest_hash=database.manifest.manifest_hash,
        requirement_registry_snapshot=snapshot,
        active_chunk_snapshot=database.plan.active_chunk_snapshot,
        expected_previous_published_epoch_id=(
            database.plan.expected_previous_published_epoch_id
        ),
    )
    database.connection.commit()
    return plan, _root_jobs_for_plan(plan, database.manifest)


def _coverage_tuple(coverage: M5RuntimeTimingCoverage) -> tuple[int | bool, ...]:
    return (
        coverage.required_expected_count,
        coverage.required_observed_count,
        coverage.required_missing_count,
        coverage.postgres_server_execution_expected_count,
        coverage.postgres_server_execution_observed_count,
        coverage.postgres_server_execution_missing_count,
        coverage.postgres_lock_wait_expected_count,
        coverage.postgres_lock_wait_observed_count,
        coverage.postgres_lock_wait_missing_count,
        coverage.postgres_wal_bytes_expected_count,
        coverage.postgres_wal_bytes_observed_count,
        coverage.postgres_wal_bytes_missing_count,
        coverage.postgres_shared_block_reads_expected_count,
        coverage.postgres_shared_block_reads_observed_count,
        coverage.postgres_shared_block_reads_missing_count,
        coverage.terminal_client_roundtrip_included,
    )


def _late_requirement_row_counts(
    connection: Connection[Any],
    *,
    epoch_id: int,
    attempt_id: str,
) -> tuple[int, ...]:
    row = connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_attempt_result_artifact
           WHERE attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_expired_attempt_return
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_attempt_execution_evidence
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_timing
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s),
          (SELECT count(*) FROM groundloop_m5_post_terminal_attempt_audit
           WHERE epoch_id = %s AND subgraph = 'requirement'
             AND attempt_id = %s)
        """,
        (
            attempt_id,
            epoch_id,
            attempt_id,
            epoch_id,
            attempt_id,
            epoch_id,
            attempt_id,
            epoch_id,
            attempt_id,
        ),
    ).fetchone()
    assert row is not None
    return tuple(map(int, row))


@pytest.mark.parametrize("cutoff", FAILURE_TRANSITION_POINTS)
def test_every_failure_cutoff_rolls_back_in_ambient_transaction_then_retries(
    d24_requirement_db: Any,
    cutoff: str,
) -> None:
    """All 19 source hooks roll back, reconnect byte-exactly, and retry cleanly."""

    epoch_id = d24_requirement_db.epoch_id
    revision = PostgresM5RuntimeStore(d24_requirement_db.connection).current_revision(
        epoch_id
    )
    before = _database_snapshot(d24_requirement_db.connection)
    seen: list[str] = []

    def inject(point: str) -> None:
        seen.append(point)
        if point == cutoff:
            raise InjectedTerminalizationFailure(point)

    with d24_requirement_db.connection.transaction():
        with pytest.raises(InjectedTerminalizationFailure, match=cutoff):
            PostgresM5RuntimeStore(
                d24_requirement_db.connection
            ).fail_typed_epoch_atomically(
                epoch_id,
                revision,
                M5RunFailureReason.INVARIANT_FAILURE,
                M5RuntimeWork(bytes_hashed=101),
                failure_injector=inject,
            )
    assert cutoff in seen

    with d24_requirement_db.reconnect() as reconnected:
        assert _database_snapshot(reconnected) == before
        retry = PostgresM5RuntimeStore(reconnected).fail_typed_epoch_atomically(
            epoch_id,
            revision,
            M5RunFailureReason.INVARIANT_FAILURE,
            M5RuntimeWork(bytes_hashed=103),
        )
        assert retry.state is M5RunState.FAILED
        assert retry.call_work == M5RuntimeWork(bytes_hashed=103)

    assert (
        PostgresM5RuntimeStore(d24_requirement_db.connection).current_revision(epoch_id)
        == revision + 1
    )


@pytest.mark.parametrize("d24_requirement_db", ["recovery_unopened"], indirect=True)
@pytest.mark.parametrize("event_kind", ("replace", "retire"))
def test_replace_and_retire_group_failures_close_their_exact_runtime_shape(
    d24_requirement_db: Any,
    event_kind: str,
) -> None:
    """R2a terminalizes both remaining self-contained group lifecycle kinds."""

    plan, jobs = _replacement_or_retirement_plan(d24_requirement_db, event_kind)
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    opened = store.open_typed_event_atomically(
        plan,
        recovery_operational_config=d24_requirement_db.operational_config,
        recovery_root_fallback_required={job.logical_job_id: False for job in jobs},
    )
    assert opened.epoch_id > 0
    revision = store.current_revision(opened.epoch_id)
    work_before_failure = store.current_event_work(opened.epoch_id)
    result = store.fail_typed_epoch_atomically(
        opened.epoch_id,
        revision,
        M5RunFailureReason.INVARIANT_FAILURE,
        M5RuntimeWork(bytes_serialized=107),
    )
    expected_work = _sum_work(
        work_before_failure,
        M5RuntimeWork(requirement_cancelled_job_count=len(jobs)),
    )
    assert result.event_work == expected_work
    assert result.call_work == M5RuntimeWork(bytes_serialized=107)
    assert store.current_revision(opened.epoch_id) == revision + 1

    closure = d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.open_work_count,
               runtime.open_scope_count, runtime.blocking_failure_count,
               (SELECT count(*) FROM groundloop_m5_semantic_job
                WHERE epoch_id = runtime.epoch_id AND job_state = 'cancelled'),
               (SELECT count(*) FROM groundloop_m5_discovery_scope
                WHERE epoch_id = runtime.epoch_id AND scope_state = 'cancelled'),
               (SELECT count(*) FROM groundloop_m5_group_deactivation
                WHERE epoch_id = runtime.epoch_id),
               (SELECT count(*) FROM groundloop_m5_group_version
                WHERE creator_epoch_id = runtime.epoch_id
                  AND lifecycle_state = 'FAILED'),
               (SELECT count(*) FROM groundloop_m5_requirement_version
                WHERE creator_epoch_id = runtime.epoch_id
                  AND lifecycle_state = 'FAILED'),
               (SELECT count(*) FROM groundloop_m5_group_family
                WHERE creator_epoch_id = runtime.epoch_id
                  AND lifecycle_state = 'FAILED')
        FROM groundloop_m5_runtime_epoch AS runtime
        WHERE runtime.epoch_id = %s
        """,
        (opened.epoch_id,),
    ).fetchone()
    expected_root_count = len(jobs)
    expected_staged_count = int(event_kind == "replace")
    assert closure == (
        "failed",
        0,
        0,
        0,
        expected_root_count,
        expected_root_count,
        1,
        expected_staged_count,
        expected_staged_count,
        0,
    )
    pending_rows = d24_requirement_db.connection.execute(
        """
        SELECT forward_scope_count, broad_reverse_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        UNION ALL
        SELECT forward_scope_count, broad_reverse_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        """,
        (opened.epoch_id, opened.epoch_id),
    ).fetchall()
    assert all(tuple(row) == (0, 0, 0, 0, revision + 1) for row in pending_rows)


def test_active_stale_failure_is_a_zero_write_conflict(
    d24_requirement_db: Any,
) -> None:
    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    lease = store.acquire_m5_job(
        d24_requirement_db.epoch_id,
        1,
        d24_requirement_db.jobs[0],
    )
    assert lease.resulting_revision == 2
    before = _database_snapshot(d24_requirement_db.connection)
    with pytest.raises(EventConflictError, match="stale typed runtime revision"):
        store.fail_typed_epoch_atomically(
            d24_requirement_db.epoch_id,
            1,
            M5RunFailureReason.INVARIANT_FAILURE,
            M5RuntimeWork(),
        )
    assert _database_snapshot(d24_requirement_db.connection) == before


@pytest.mark.parametrize(
    "direct_update_kind",
    ("policy_change", "document_insert", "document_delete", "document_replace"),
)
def test_fresh_direct_update_declaration_is_rejected_without_failure_writes(
    d24_requirement_db: Any,
    direct_update_kind: str,
) -> None:
    """The R2a guard leaves direct/application failure composition to R2b."""

    original = _database_snapshot(d24_requirement_db.connection)
    with pytest.raises(RollBackDirectKindFixture):
        with d24_requirement_db.connection.transaction():
            d24_requirement_db.connection.execute(
                """
                UPDATE groundloop_m5_update SET update_kind = %s
                WHERE epoch_id = %s
                """,
                (direct_update_kind, d24_requirement_db.epoch_id),
            )
            before_rejection = _database_snapshot_in_transaction(
                d24_requirement_db.connection
            )
            with pytest.raises(
                InvalidEventError,
                match="direct failure composition outside R2a",
            ):
                PostgresM5RuntimeStore(
                    d24_requirement_db.connection
                ).fail_typed_epoch_atomically(
                    d24_requirement_db.epoch_id,
                    1,
                    M5RunFailureReason.INVARIANT_FAILURE,
                    M5RuntimeWork(),
                )
            assert (
                _database_snapshot_in_transaction(d24_requirement_db.connection)
                == before_rejection
            )
            raise RollBackDirectKindFixture
    assert _database_snapshot(d24_requirement_db.connection) == original


def test_expired_root_output_archives_five_rows_after_real_failure_terminalization(
    d24_requirement_db: Any,
) -> None:
    """Production failure resolves a retryable root before C4 late return."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    job = d24_requirement_db.jobs[0]
    original = store.acquire_m5_job(epoch_id, 1, job)
    assert original.attempt is not None and original.lease_expires_at is not None
    _wait_until_expired(d24_requirement_db.connection, original.lease_expires_at)
    takeover = store.acquire_m5_job(epoch_id, 2, job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 3
    discovery, output = _root_result_and_output(
        d24_requirement_db,
        job=job,
        attempt=original.attempt,
    )
    retryable = store.mark_m5_retryable_failure(
        epoch_id,
        3,
        takeover,
        _sha("d24-r2a-root-retryable"),
        _forward_attempt_work(),
        _attempt_timing(),
    )
    assert retryable.resulting_revision == 4 and not retryable.exact_replay
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)

    def old_output(connection: Connection[Any], expected_revision: int) -> Any:
        return PostgresM5RuntimeStore(connection).stage_m5_discovery_result_atomically(
            epoch_id,
            expected_revision,
            original,
            job,
            discovery,
            output,
            M5ExecutionEvidenceDisposition.RETURNED,
            _forward_attempt_work(),
            _attempt_timing(),
            eligible_snapshot_exhausted=True,
        )

    for rejected_revision in (3, 4):
        before_rejection = _database_snapshot(d24_requirement_db.connection)
        with pytest.raises(
            EventConflictError,
            match="nonrunning successor requires terminal event",
        ):
            old_output(d24_requirement_db.connection, rejected_revision)
        assert _database_snapshot(d24_requirement_db.connection) == before_rejection

    work_before_failure = store.current_event_work(epoch_id)
    terminal = store.fail_typed_epoch_atomically(
        epoch_id,
        4,
        M5RunFailureReason.RETRY_EXHAUSTED,
        M5RuntimeWork(bytes_hashed=109),
    )
    assert terminal.logical_result_hash is not None
    assert terminal.event_work == _sum_work(
        work_before_failure,
        M5RuntimeWork(requirement_cancelled_job_count=1),
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision,
               runtime.open_work_count, runtime.open_scope_count,
               job.job_state, job.archive_reason, job.completed_revision,
               scope.scope_state, scope.closed_revision
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS scope
          ON scope.epoch_id = job.epoch_id
         AND scope.root_job_id = job.logical_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, job.logical_job_id),
    ).fetchone() == (
        "failed",
        5,
        0,
        0,
        "cancelled",
        "epoch_failed",
        5,
        "cancelled",
        5,
    )

    terminal_before = _snapshot_tables(
        d24_requirement_db.connection,
        _FROZEN_TERMINAL_TABLES,
    )
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection,
        _LATE_SEMANTIC_TABLES,
    )

    receipt = old_output(d24_requirement_db.connection, 5)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert not receipt.exact_replay and receipt.resulting_revision == 5
    assert receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal.logical_result_hash
    assert (
        _snapshot_tables(
            d24_requirement_db.connection,
            _FROZEN_TERMINAL_TABLES,
        )
        == terminal_before
    )
    assert (
        _snapshot_tables(
            d24_requirement_db.connection,
            _LATE_SEMANTIC_TABLES,
        )
        == semantic_before
    )
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=original.attempt.attempt_id,
    ) == (1, 1, 1, 1, 1)

    frozen = _database_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = old_output(reconnected, 5)
        assert replay.exact_replay
        assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
        assert (
            replay.current_terminal_logical_result_hash == terminal.logical_result_hash
        )
    assert _database_snapshot(d24_requirement_db.connection) == frozen


def test_expired_verifier_output_archives_five_rows_after_real_terminalization(
    d24_requirement_db: Any,
) -> None:
    """Production failure resolves a retryable verifier before C4 late return."""

    store, fixture = _prepare_verifier_attempt(d24_requirement_db)
    epoch_id = d24_requirement_db.epoch_id
    assert fixture.lease.attempt is not None
    assert fixture.lease.lease_expires_at is not None
    _wait_until_expired(
        d24_requirement_db.connection,
        fixture.lease.lease_expires_at,
    )
    takeover = store.acquire_m5_job(epoch_id, 5, fixture.job)
    assert takeover.disposition is M5AcquisitionDisposition.DISPATCH_TAKEOVER
    assert takeover.attempt is not None and takeover.resulting_revision == 6
    retryable = store.mark_m5_retryable_failure(
        epoch_id,
        6,
        takeover,
        _sha("d24-r2a-verifier-retryable"),
        _verifier_attempt_work(M5ExecutionEvidenceDisposition.RETURNED),
        _attempt_timing(),
    )
    assert retryable.resulting_revision == 7 and not retryable.exact_replay
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=fixture.lease.attempt.attempt_id,
    ) == (0, 0, 0, 0, 0)
    assert _verifier_semantic_closure_counts(
        d24_requirement_db,
        fixture,
    ) == (0, 0, 0, 0)

    def old_output(connection: Connection[Any], expected_revision: int) -> Any:
        return _complete_verifier(
            PostgresM5RuntimeStore(connection),
            d24_requirement_db,
            fixture,
            expected_revision=expected_revision,
            disposition=M5ExecutionEvidenceDisposition.RETURNED,
            timing=_attempt_timing(),
        )

    for rejected_revision in (6, 7):
        before_rejection = _database_snapshot(d24_requirement_db.connection)
        with pytest.raises(
            EventConflictError,
            match="nonrunning successor requires terminal event",
        ):
            old_output(d24_requirement_db.connection, rejected_revision)
        assert _database_snapshot(d24_requirement_db.connection) == before_rejection

    work_before_failure = store.current_event_work(epoch_id)
    terminal = store.fail_typed_epoch_atomically(
        epoch_id,
        7,
        M5RunFailureReason.VERIFIER_UNAVAILABLE,
        M5RuntimeWork(bytes_serialized=113),
    )
    assert terminal.logical_result_hash is not None
    assert terminal.event_work == _sum_work(
        work_before_failure,
        M5RuntimeWork(requirement_cancelled_job_count=1),
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT runtime.runtime_state, runtime.revision,
               runtime.open_work_count, runtime.open_scope_count,
               job.job_state, job.archive_reason, job.completed_revision,
               parent.scope_state, parent.closed_revision
        FROM groundloop_m5_runtime_epoch AS runtime
        JOIN groundloop_m5_semantic_job AS job USING (epoch_id)
        JOIN groundloop_m5_discovery_scope AS parent
          ON parent.epoch_id = job.epoch_id
         AND parent.root_job_id = job.parent_job_id
        WHERE runtime.epoch_id = %s AND job.logical_job_id = %s
        """,
        (epoch_id, fixture.job.logical_job_id),
    ).fetchone() == (
        "failed",
        8,
        0,
        0,
        "cancelled",
        "epoch_failed",
        8,
        "closed_active",
        4,
    )

    terminal_before = _snapshot_tables(
        d24_requirement_db.connection,
        _FROZEN_TERMINAL_TABLES,
    )
    semantic_before = _snapshot_tables(
        d24_requirement_db.connection,
        _LATE_SEMANTIC_TABLES,
    )

    receipt = old_output(d24_requirement_db.connection, 8)
    assert receipt.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
    assert not receipt.exact_replay and receipt.resulting_revision == 8
    assert receipt.transition_anchor is None
    assert receipt.current_terminal_logical_result_hash == terminal.logical_result_hash
    assert (
        _snapshot_tables(
            d24_requirement_db.connection,
            _FROZEN_TERMINAL_TABLES,
        )
        == terminal_before
    )
    assert (
        _snapshot_tables(
            d24_requirement_db.connection,
            _LATE_SEMANTIC_TABLES,
        )
        == semantic_before
    )
    assert _verifier_semantic_closure_counts(
        d24_requirement_db,
        fixture,
    ) == (0, 0, 0, 0)
    assert _late_requirement_row_counts(
        d24_requirement_db.connection,
        epoch_id=epoch_id,
        attempt_id=fixture.lease.attempt.attempt_id,
    ) == (1, 1, 1, 1, 1)

    frozen = _database_snapshot(d24_requirement_db.connection)
    with d24_requirement_db.reconnect() as reconnected:
        replay = old_output(reconnected, 8)
        assert replay.exact_replay
        assert replay.disposition is M5RequirementReturnDisposition.EXPIRED_POSTTERMINAL
        assert (
            replay.current_terminal_logical_result_hash == terminal.logical_result_hash
        )
    assert _database_snapshot(d24_requirement_db.connection) == frozen


def test_register_group_failure_freezes_exact_terminal_accounting_and_replays(
    d24_requirement_db: Any,
) -> None:
    """The production R2a path closes one real register-group epoch exactly."""

    store = PostgresM5RuntimeStore(d24_requirement_db.connection)
    epoch_id = d24_requirement_db.epoch_id
    expected_revision = store.current_revision(epoch_id)
    work_before_failure = store.current_event_work(epoch_id)
    published_before_failure = _strict_published_snapshot(d24_requirement_db.connection)
    call_work = M5RuntimeWork(
        requirement_forward_retrieval_call_count=3,
        bytes_hashed=41,
        bytes_serialized=43,
        embedding_model_call_count=2,
        embedding_input_token_count=47,
    )
    cancellation_work = M5RuntimeWork(
        requirement_cancelled_job_count=len(d24_requirement_db.jobs)
    )
    expected_event_work = _sum_work(work_before_failure, cancellation_work)

    result = store.fail_typed_epoch_atomically(
        epoch_id,
        expected_revision,
        M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
        call_work,
    )

    assert result.state is M5RunState.FAILED
    assert result.replayed_outcome is None
    assert result.failure_reason is M5RunFailureReason.RETRIEVAL_UNAVAILABLE
    assert result.event_work == expected_event_work
    assert result.call_work == call_work
    assert result.call_work != result.event_work
    assert result.event_timing == M5RuntimeTiming()
    assert result.call_timing == M5RuntimeTiming()
    assert result.call_timing_coverage == M5RuntimeTimingCoverage.single_point(
        None,
        terminal_client_roundtrip_included=False,
    )
    assert _coverage_tuple(result.event_timing_coverage) == (
        2,
        0,
        2,
        2,
        0,
        2,
        2,
        0,
        2,
        2,
        0,
        2,
        2,
        0,
        2,
        False,
    )
    assert _coverage_tuple(result.call_timing_coverage) == (
        1,
        0,
        1,
        1,
        0,
        1,
        1,
        0,
        1,
        1,
        0,
        1,
        1,
        0,
        1,
        False,
    )
    assert store.current_revision(epoch_id) == expected_revision + 1
    assert store.current_event_work(epoch_id) == expected_event_work
    assert store.current_event_timing(epoch_id) == (
        result.event_timing,
        result.event_timing_coverage,
    )
    assert _strict_published_snapshot(d24_requirement_db.connection) == (
        published_before_failure
    )

    terminal_row = d24_requirement_db.connection.execute(
        """
        SELECT base.revision, base.structural_status, base.semantic_status,
               base.evaluation_state, base.publication_mode, base.sealed_at,
               runtime.revision, runtime.runtime_state,
               runtime.open_work_count, runtime.open_scope_count,
               runtime.blocking_failure_count, runtime.terminal_at,
               accumulator.updated_revision, accumulator.terminalized,
               timing.updated_revision, timing.terminalized,
               timing.pending_contribution_kind, timing.pending_source_id,
               timing.pending_contribution_key_digest,
               timing.pending_anchor_revision
        FROM groundloop_epoch AS base
        JOIN groundloop_m5_runtime_epoch AS runtime USING (epoch_id)
        JOIN groundloop_m5_runtime_work_accumulator AS accumulator USING (epoch_id)
        JOIN groundloop_m5_runtime_timing_accumulator AS timing USING (epoch_id)
        WHERE base.epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert terminal_row is not None
    assert terminal_row[:12] == (
        expected_revision + 1,
        "failed",
        "failed",
        "failed",
        "provisional",
        None,
        expected_revision + 1,
        "failed",
        0,
        0,
        0,
        terminal_row[11],
    )
    assert terminal_row[11] is not None
    assert terminal_row[12:] == (
        expected_revision + 1,
        True,
        expected_revision + 1,
        True,
        None,
        None,
        None,
        None,
    )

    work_rows = d24_requirement_db.connection.execute(
        """
        SELECT work_kind, work_digest
        FROM groundloop_m5_runtime_work
        WHERE structural_event_id = %s
        ORDER BY work_kind COLLATE "C"
        """,
        (d24_requirement_db.plan.structural_event_id,),
    ).fetchall()
    assert work_rows == [
        ("call", call_work.work_digest),
        ("event", expected_event_work.work_digest),
    ]
    contribution_rows = d24_requirement_db.connection.execute(
        """
        SELECT contribution_kind, applied_revision,
               requirement_cancelled_job_count, work_digest
        FROM groundloop_m5_runtime_work_contribution
        WHERE epoch_id = %s
        ORDER BY applied_revision, contribution_kind COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    assert contribution_rows == [
        ("structural_open", expected_revision, 0, work_before_failure.work_digest),
        (
            "cancellation",
            expected_revision + 1,
            len(d24_requirement_db.jobs),
            cancellation_work.work_digest,
        ),
        (
            "epoch_failure",
            expected_revision + 1,
            0,
            M5RuntimeWork().work_digest,
        ),
    ]

    job_rows = d24_requirement_db.connection.execute(
        """
        SELECT job_state, archive_reason, completed_revision,
               cancelled_by_event_id, cancelled_by_epoch_id,
               cancellation_reason
        FROM groundloop_m5_semantic_job
        WHERE epoch_id = %s
        ORDER BY logical_job_id COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    assert job_rows == [
        (
            "cancelled",
            "epoch_failed",
            expected_revision + 1,
            d24_requirement_db.plan.structural_event_id,
            epoch_id,
            "epoch_failed",
        )
    ]
    scope_rows = d24_requirement_db.connection.execute(
        """
        SELECT scope_state, closed_revision
        FROM groundloop_m5_discovery_scope
        WHERE epoch_id = %s
        ORDER BY root_job_id COLLATE "C"
        """,
        (epoch_id,),
    ).fetchall()
    assert scope_rows == [("cancelled", expected_revision + 1)]
    pending_rows = d24_requirement_db.connection.execute(
        """
        SELECT forward_scope_count, broad_reverse_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_owner_pending_counter
        WHERE epoch_id = %s
        UNION ALL
        SELECT forward_scope_count, broad_reverse_scope_count,
               verifier_job_count, blocking_failure_count, updated_revision
        FROM groundloop_m5_answer_pending_counter
        WHERE epoch_id = %s
        """,
        (epoch_id, epoch_id),
    ).fetchall()
    assert len(pending_rows) == 2
    assert all(
        tuple(row) == (0, 0, 0, 0, expected_revision + 1) for row in pending_rows
    )
    assert d24_requirement_db.connection.execute(
        """
        SELECT
          (SELECT count(*) FROM groundloop_m5_group_family
           WHERE creator_epoch_id = %s AND lifecycle_state = 'FAILED'),
          (SELECT count(*) FROM groundloop_m5_group_version
           WHERE creator_epoch_id = %s AND lifecycle_state = 'FAILED'),
          (SELECT count(*) FROM groundloop_m5_requirement_version
           WHERE creator_epoch_id = %s AND lifecycle_state = 'FAILED')
        """,
        (epoch_id, epoch_id, epoch_id),
    ).fetchone() == (1, 1, 1)

    coverage_row = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
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
        FROM groundloop_m5_event_timing_coverage
        WHERE structural_event_id = %s
        """,
        (d24_requirement_db.plan.structural_event_id,),
    ).fetchone()
    assert coverage_row is not None
    assert tuple(coverage_row) == _coverage_tuple(result.event_timing_coverage)
    accumulator_coverage = d24_requirement_db.connection.execute(
        """
        SELECT required_expected_count, required_observed_count,
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
               false
        FROM groundloop_m5_runtime_timing_accumulator
        WHERE epoch_id = %s
        """,
        (epoch_id,),
    ).fetchone()
    assert accumulator_coverage is not None
    assert tuple(accumulator_coverage) == tuple(coverage_row)

    frozen = _database_snapshot(d24_requirement_db.connection)
    for replay_revision in (expected_revision, expected_revision + 1):
        replay = store.fail_typed_epoch_atomically(
            epoch_id,
            replay_revision,
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RuntimeWork(bytes_hashed=999),
        )
        assert replay.replayed_outcome is M5ReplayedOutcome.FAILED
        assert replay.event_work == result.event_work
        assert replay.call_work.is_zero
        assert replay.event_timing == result.event_timing
        assert replay.event_timing_coverage == result.event_timing_coverage
        assert _database_snapshot(d24_requirement_db.connection) == frozen

    with pytest.raises(
        EventConflictError,
        match="expected revision is newer than durable runtime",
    ):
        store.fail_typed_epoch_atomically(
            epoch_id,
            expected_revision + 2,
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RuntimeWork(),
        )
    assert _database_snapshot(d24_requirement_db.connection) == frozen

    with pytest.raises(
        EventConflictError,
        match="another failure reason",
    ):
        store.fail_typed_epoch_atomically(
            epoch_id,
            expected_revision + 1,
            M5RunFailureReason.VERIFIER_UNAVAILABLE,
            M5RuntimeWork(),
        )
    assert _database_snapshot(d24_requirement_db.connection) == frozen

    with d24_requirement_db.reconnect() as reconnected:
        reconnect_replay = PostgresM5RuntimeStore(
            reconnected
        ).fail_typed_epoch_atomically(
            epoch_id,
            expected_revision + 1,
            M5RunFailureReason.RETRIEVAL_UNAVAILABLE,
            M5RuntimeWork(bytes_hashed=1009),
        )
        assert reconnect_replay.replayed_outcome is M5ReplayedOutcome.FAILED
        assert reconnect_replay.call_work.is_zero
    assert _database_snapshot(d24_requirement_db.connection) == frozen
